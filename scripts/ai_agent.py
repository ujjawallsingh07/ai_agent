import os
import openai
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import OpenAI
from langgraph.graph import StateGraph, END
from pydantic import BaseModel

# Load API key from environment
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

if not openai_api_key:
    raise ValueError("❌ OPENAI_API_KEY is missing. Set it in .env or as an environment variable.")

# Define AI Model
llm = OpenAI(openai_api_key=openai_api_key, temperature=0)


class CleaningState(BaseModel):
    """
    State schema defining input and output for the LangGraph agent.
    """
    input_text: str
    structured_response: str = ""


class AIAgent:
    def _init_(self):
        self.graph = self.create_graph()

    def create_graph(self):
        """Creates and returns a LangGraph agent graph with state management."""
        graph = StateGraph(CleaningState)

        # Agent logic to process input and return a structured response
        def agent_logic(state: CleaningState) -> CleaningState:
            response = llm.invoke(state.input_text)
            return CleaningState(input_text=state.input_text, structured_response=response)

        graph.add_node("cleaning_agent", agent_logic)
        graph.add_edge("cleaning_agent", END)
        graph.set_entry_point("cleaning_agent")
        return graph.compile()

    def process_data(self, df, batch_size=20):
        """
        Processes data in batches to avoid OpenAI's token limit.
        Identifies missing values, removes duplicates, formats text, etc.
        Returns the cleaned data as structured text.
        """
        cleaned_responses = []

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i + batch_size]
            prompt = f"""
You are an AI Data Cleaning Agent. Analyze the dataset:
{df_batch.to_string()}
Identify missing values, choose the best imputation strategy (mean, mode, median), remove duplicates, and format text correctly.
Return the cleaned data as structured text.
"""
            state = CleaningState(input_text=prompt, structured_response="")
            response = self.graph.invoke(state)
            if isinstance(response, dict):
                response = CleaningState(**response)
            cleaned_responses.append(response.structured_response)

        return "\n".join(cleaned_responses)