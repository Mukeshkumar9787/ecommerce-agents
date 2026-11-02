"""Multi-Agent Procurement StateGraph definition and compilation."""

import logging
from typing import Any, List, Optional
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from src.agents.inventory_analyst import inventory_analyst_node
from src.agents.po_manager import po_manager_node
from src.agents.supervisor import supervisor_node, supervisor_router
from src.config import get_settings
from src.schemas.state import ProcurementState

logger = logging.getLogger(__name__)


def build_procurement_graph() -> StateGraph:
    """
    Construct the StateGraph with supervisor, inventory_analyst, and po_manager nodes.

    Routing topology:
    START -> supervisor
    supervisor -> (conditional) -> inventory_analyst | po_manager | END
    inventory_analyst -> supervisor
    po_manager -> supervisor
    """
    workflow = StateGraph(ProcurementState)

    # Add workflow agent nodes
    workflow.add_node("supervisor", supervisor_node)
    workflow.add_node("inventory_analyst", inventory_analyst_node)
    workflow.add_node("po_manager", po_manager_node)

    # Edge configuration
    workflow.add_edge(START, "supervisor")
    workflow.add_conditional_edges(
        "supervisor",
        supervisor_router,
        {
            "inventory_analyst": "inventory_analyst",
            "po_manager": "po_manager",
            END: END,
        },
    )
    workflow.add_edge("inventory_analyst", "supervisor")
    workflow.add_edge("po_manager", "supervisor")

    return workflow


_POSTGRES_CONN = None


def get_postgres_checkpointer(conn_string: Optional[str] = None) -> BaseCheckpointSaver:
    """
    Attempt to initialize and setup PostgresSaver checkpointer.
    Falls back to in-memory checkpointer if PostgreSQL is unreachable or unconfigured.
    """
    global _POSTGRES_CONN
    settings = get_settings()
    uri = conn_string or settings.POSTGRES_URI

    try:
        from langgraph.checkpoint.postgres import PostgresSaver
        import psycopg

        if _POSTGRES_CONN is None or _POSTGRES_CONN.closed:
            _POSTGRES_CONN = psycopg.connect(uri, connect_timeout=3, autocommit=True)
            checkpointer = PostgresSaver(_POSTGRES_CONN)
            checkpointer.setup()
            logger.info("Successfully connected and initialized PostgresSaver checkpointer.")
            return checkpointer
        return PostgresSaver(_POSTGRES_CONN)
    except Exception as exc:
        logger.warning(
            "PostgreSQL checkpointer unavailable at '%s' (%s). Falling back to MemorySaver.",
            uri,
            exc,
        )
        return MemorySaver()


_LANGFUSE_CLIENT = None


def get_langfuse_callback() -> Optional[Any]:
    """Instantiate Langfuse CallbackHandler if API keys are configured."""
    global _LANGFUSE_CLIENT
    settings = get_settings()
    if settings.LANGFUSE_PUBLIC_KEY and settings.LANGFUSE_SECRET_KEY:
        try:
            import os
            # Set environment variables expected by Langfuse SDK
            os.environ["LANGFUSE_PUBLIC_KEY"] = settings.LANGFUSE_PUBLIC_KEY
            os.environ["LANGFUSE_SECRET_KEY"] = settings.LANGFUSE_SECRET_KEY
            os.environ["LANGFUSE_HOST"] = settings.LANGFUSE_HOST

            from langfuse import Langfuse
            if _LANGFUSE_CLIENT is None:
                _LANGFUSE_CLIENT = Langfuse(
                    public_key=settings.LANGFUSE_PUBLIC_KEY,
                    secret_key=settings.LANGFUSE_SECRET_KEY,
                    host=settings.LANGFUSE_HOST,
                )

            try:
                from langfuse.langchain import CallbackHandler
                handler = CallbackHandler()
            except ImportError:
                from langfuse.callback import CallbackHandler  # type: ignore
                handler = CallbackHandler()

            logger.info("Initialized Langfuse CallbackHandler for telemetry.")
            return handler
        except Exception as exc:
            logger.warning("Could not initialize Langfuse callback: %s", exc)
            return None
    return None


def flush_langfuse():
    """Flush pending Langfuse telemetry events to the cloud server."""
    global _LANGFUSE_CLIENT
    if _LANGFUSE_CLIENT is not None:
        try:
            _LANGFUSE_CLIENT.flush()
            logger.info("Flushed Langfuse telemetry traces.")
        except Exception as exc:
            logger.debug("Langfuse flush warning: %s", exc)



def create_procurement_app(
    checkpointer: Optional[BaseCheckpointSaver] = None,
    interrupt_after: Optional[List[str]] = None,
):
    """
    Compile the procurement multi-agent StateGraph with checkpointing and human-in-the-loop interruption.

    Args:
        checkpointer: Custom checkpointer (e.g. MemorySaver for testing or PostgresSaver for production).
        interrupt_after: List of node names after which to pause execution. Defaults to ["po_manager"].

    Returns:
        Compiled LangGraph application.
    """
    workflow = build_procurement_graph()

    saver = checkpointer
    if saver is None:
        saver = get_postgres_checkpointer()

    interrupt_nodes = interrupt_after if interrupt_after is not None else ["po_manager"]

    app = workflow.compile(
        checkpointer=saver,
        interrupt_after=interrupt_nodes,
    )
    logger.info("Compiled Procurement Multi-Agent Application with interrupt_after=%s", interrupt_nodes)
    return app
