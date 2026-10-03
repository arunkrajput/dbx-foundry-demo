"""
CONSUME stage: a Microsoft Foundry prompt agent that uses EVERY registered data product as tools:
bronze operational, silver reference and gold consumer-aligned.

    pip install -r requirements.txt
    cp .env.example .env     # fill in values
    az login                 # Foundry auth (DefaultAzureCredential)
    python foundry_agent.py                      # interactive
    python foundry_agent.py --demo               # runs the test questions

Two identities are in play, on purpose:
  * You (az login)            -> allowed to create and run the agent in Foundry
  * Databricks service principal (.env) -> what the agent is allowed to SEE in Unity Catalog
"""
import argparse
import json
import os

from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import FunctionTool, PromptAgentDefinition
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from openai.types.responses.response_input_param import FunctionCallOutput

from dataproduct import DataProduct

load_dotenv()

INSTRUCTIONS = """You are a retail banking data assistant. You answer ONLY from the data product tools.
Choosing a product (each tool says its layer and when to use it):
- GOLD consumer-aligned (customer_accounts): default for counts, balances, segment or regional analysis.
- BRONZE operational (customer_accounts_ops): what the source system sent, latest status, data issues.
  Its values are unvalidated; rows with _dq_status = 'failed' broke the contract. Never use it for figures.
- SILVER reference (ref_branch): what a branch code means, its region, whether it is active.
Rules:
1. Before quoting any figure, call the health tool of the product you use. If trustworthy is false, say so up
   front and why (stale, low quality, deprecated, or an unhealthy input product), then answer, clearly labelled.
2. Prefer the aggregate tool over reading rows. Never dump more rows than needed.
3. Values that are masked (e.g. ******-**-1234, initials) or NULL are hidden by policy for your identity.
   Say they are restricted; never guess or reconstruct them.
4. End every answer with: Source: <product id>@<version>, last load <last_load_at>.
"""

DEMO_QUESTIONS = [
    "What is this customer accounts data product and who owns it?",
    "Is the data fresh enough to use today?",
    "How many accounts are there per segment?",
    "What is the total balance for private banking customers?",
    "Show me the IC number and name for 3 private segment customers in branch KL01.",
    "How many accounts did the source send with data quality issues, and what are the most common problems?",
    "Which region is branch KC01 in, and is it still active?",
    "How many accounts are there per region?",
]


def build_agent(project: AIProjectClient, products: list[DataProduct]):
    tools = [FunctionTool(name=t["name"], description=t["description"],
                          parameters=t["parameters"], strict=False)
             for p in products for t in p.as_agent_tools()]
    return project.agents.create_version(
        agent_name="retail-banking-data-agent",
        definition=PromptAgentDefinition(
            model=os.environ["FOUNDRY_MODEL_DEPLOYMENT_NAME"],
            instructions=INSTRUCTIONS,
            tools=tools,
        ),
    )


def ask(openai, agent, conversation_id: str, router: dict[str, DataProduct], question: str,
        trace: bool = True) -> str:
    agent_ref = {"agent_reference": {"name": agent.name, "type": "agent_reference"}}
    response = openai.responses.create(input=question, conversation=conversation_id, extra_body=agent_ref)

    # Keep executing tool calls until the model produces a final answer
    for _ in range(8):
        calls = [item for item in response.output if item.type == "function_call"]
        if not calls:
            break
        outputs = []
        for call in calls:
            product = router.get(call.name)
            result = (product.dispatch(call.name, call.arguments) if product
                      else json.dumps({"error": f"unknown tool {call.name}"}))
            if trace:
                print(f"  -> {call.name}({call.arguments})\n     <- {result[:300]}{'...' if len(result) > 300 else ''}")
            outputs.append(FunctionCallOutput(type="function_call_output", call_id=call.call_id, output=result))
        response = openai.responses.create(input=outputs, conversation=conversation_id, extra_body=agent_ref)
    return response.output_text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--keep-agent", action="store_true", help="don't delete the agent version at exit")
    a = ap.parse_args()

    # Discovery: every non-retired product the agent's Databricks identity can see in the registry.
    # Authenticates with DATABRICKS_HOST / CLIENT_ID / CLIENT_SECRET from the env.
    products = DataProduct.discover()
    router = {t["name"]: p for p in products for t in p.as_agent_tools()}
    for p in products:
        print(f"  {p.layer:<7} {p.product_type:<17} {p.id} v{p.reg['version']} -> {p.table}")

    project = AIProjectClient(endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"], credential=DefaultAzureCredential())
    openai = project.get_openai_client()
    agent = build_agent(project, products)
    conversation = openai.conversations.create()
    print(f"Agent {agent.name} v{agent.version} ready.\n")

    try:
        questions = DEMO_QUESTIONS if a.demo else iter(lambda: input("you> "), "exit")
        for q in questions:
            if a.demo:
                print(f"you> {q}")
            print(f"agent> {ask(openai, agent, conversation.id, router, q)}\n")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        openai.conversations.delete(conversation_id=conversation.id)
        if not a.keep_agent:
            project.agents.delete_version(agent_name=agent.name, agent_version=agent.version)


if __name__ == "__main__":
    main()
