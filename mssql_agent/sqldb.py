import logging
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.pool").setLevel(logging.INFO)
from langchain_community.utilities.sql_database import SQLDatabase
from typing_extensions import TypedDict,Annotated
from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool
from langchain_core.prompts import ChatPromptTemplate
from colorama import Fore, Style, init
from langchain_openai import ChatOpenAI
init(autoreset=True)  # ensures colors reset after each print
import asyncio
import re
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError



class QueryOutput(TypedDict):
    """Generated SQL query."""

    query: Annotated[str, ..., "Syntactically valid SQL query."]

class SQLConnector:
    """Optimized SQL Server (PostgreSQL dialect) connection manager with connection pooling and retry."""

    def __init__(self, username: str, password: str, host: str, port: int, database: str):
        self.username = username
        self.password = password.replace("@", "%40")  # escape special chars
        self.host = host
        self.port = port
        self.database = database
        self._engine = None
        self._db = None

    def _create_engine(self):
        """Create a SQLAlchemy engine with connection pooling."""
        uri = f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        engine = create_engine(
            uri,
            pool_pre_ping=True,      # checks if connection is alive
            pool_size=5,             # maintain 5 connections
            max_overflow=10,         # allow up to 10 more temporary ones
            pool_recycle=1800,       # recycle every 30 minutes
            pool_timeout=30,         # 30s timeout for waiting connections
        )
        return engine

    def connect(self):
        """Establish a pooled SQL connection if not already active."""
        if self._db is None:
            if self._engine is None:
                self._engine = self._create_engine()
            self._db = SQLDatabase.from_uri(self._engine.url, schema="dbo")
        return self._db

    def promptemp(self):   
        system_message = """
        You are an expert SQL (PostgreSQL) query generator.

        Given an input question, create a syntactically correct {dialect} query to
        run to help find the answer. Unless the user specifies in his question a
        specific number of examples they wish to obtain, always limit your query to
        at most {top_k} results. You can order the results by a relevant column to
        return the most interesting examples in the database.

        Never query for all the columns from a specific table, only ask for a few
        relevant columns given the question.

        Pay attention to use only the column names that you can see in the schema
        description. Be careful to not query for columns that do not exist. Also,
        pay attention to which column is in which table.

        Rules:
        - Always generate PostgreSQL queries.
        - Use `LIMIT {top_k}` to restrict the number of rows.
        - Do not use `TOP`, `RETURNING` (unless needed for INSERT), or any T-SQL-specific clauses.
        - Only use the columns and tables listed in the schema.
        - Never select all columns (*), only the required ones.
        - Ensure syntax is valid for PostgreSQL.

        Only use the following tables:
        Table Names: {table_info}


        """

        user_prompt = "Question: {input}"

        query_prompt_template = ChatPromptTemplate(
            [("system", system_message), ("user", user_prompt)]
        )
    

        return query_prompt_template
    
    

    def write_query(self,question,llm):
        """Generate SQL query to fetch information."""
        db = self.connect()
        query_prompt_template = self.promptemp()
        prompt = query_prompt_template.invoke(
            {
                "dialect": db.dialect,
                "top_k": 10,
                "table_info": db.get_table_info(),
                "input": question
            }
        )
        print(prompt)
        structured_llm = llm.with_structured_output(QueryOutput)
        result = structured_llm.invoke(prompt)
        return result
    
    def execute_query(self,query):
        """Execute SQL query."""
        db = self.connect()
        execute_query_tool = QuerySQLDatabaseTool(db=db)
        sqlresult =  execute_query_tool.invoke(query)

        if isinstance(sqlresult, str) and sqlresult.lower().startswith("error:"):
            raise Exception(sqlresult)
        return sqlresult


    async def invoke_streaming(self, question, llm:ChatOpenAI):

        attempt = 0
        querygenbyllm = None  # initialize
        max_retries = 1
        db = self.connect()

        while attempt <= max_retries:
            try:
                # Generate query (first attempt or feedback)
                if attempt == 0:
                    querygenbyllm = self.write_query(question, llm)
                    
                else:
                    # regenerate query based on last error
                    feedback_prompt = (
                        f"The previously generated SQL query failed:\n{querygenbyllm}\n"
                        f"Error message: {last_error}\n"
                        f"Tables/columns allowed: {db.get_table_info()}\n"
                        f"Please generate a corrected SQL query for the same user question:\n{question}"
                    )
                    structured_llm = llm.with_structured_output(QueryOutput)
                    querygenbyllm = structured_llm.invoke(feedback_prompt)
                    


                sql_text = querygenbyllm["query"] if isinstance(querygenbyllm, dict) else str(querygenbyllm)
                print(Fore.GREEN + f'Generated SQL:\n"{sql_text}"' + Style.RESET_ALL)

                # Execute SQL
                try:
                    query_values = self.execute_query(sql_text)
                    print(Fore.RED + f'Query Result:\n"{query_values}"' + Style.RESET_ALL)
                except Exception as sql_error:
                    last_error = str(sql_error)
                    print(Fore.RED + f'SQL Execution failed:\n"{last_error}"' + Style.RESET_ALL)
                    attempt += 1
                    if attempt > max_retries:
                        # ✅ fallback if retries exhausted
                        fallback_prompt = (
                            f"The user asked: {question}\n"
                            f"However, the system could not retrieve an answer from the database "
                            f"after {max_retries} attempts.\n"
                            "Please provide a polite, general response that acknowledges the failure "
                            "without exposing technical details, and suggest the user try rephrasing."
                        )
                        async for token in llm.astream(fallback_prompt):
                            yield token.content

                        return
                    continue  # retry loop

                # Only if SQL succeeded, generate streaming answer
                answer_prompt = (
                    "Given the following user question, corresponding SQL query, "
                    "and SQL result, answer the user question.\n\n"
                    f"Question: {question}\n"
                    f"SQL Query: {sql_text}\n"
                    f"SQL Result: {query_values}"
                )
                async for token in llm.astream(answer_prompt):
                    yield token.content

                return
            
            except Exception as e:
                # Catch unexpected errors in query generation
                last_error = str(e)
                print(Fore.RED + f'Attempt {attempt+1} failed with error:\n"{last_error}"' + Style.RESET_ALL)
                attempt += 1
            # 🚨 If loop is exhausted without success (e.g. DB down, LLM issue, etc.)
            
        fallback_prompt = (
            f"The user asked: {question}\n\n"
            f"The system failed after {max_retries+1} attempts.\n"
            f"Last recorded error was:\n{last_error}\n\n"
            "Please summarize this error into a polite and general response for the user, "
            "without exposing technical details, but still acknowledging that something went wrong. "
            "Suggest they try again later or rephrase their request."
        )

        try:
            async for token in llm.astream(fallback_prompt):
                yield token.content
        except Exception:
            yield "Sorry, something went wrong while processing your request. Please try again later."
        
