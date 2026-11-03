"""Unified entrypoint to start all autonomous procurement agent worker daemons."""

import logging
import multiprocessing
import signal
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("agent_orchestrator")


def run_agent_server_worker():
    """Start unified Agent Real-Time Traceability & Embedded ERP server in separate process."""
    import os
    os.environ["IS_ERP_SERVER_PROCESS"] = "true"
    import uvicorn
    from src.config import get_settings
    settings = get_settings()
    port = int(getattr(settings, "PORT", 8002))
    logger.info("Starting Agent Traceability & Embedded ERP Server on port %d...", port)
    uvicorn.run("src.server.app:app", host="0.0.0.0", port=port, log_level="info")


# Backward compatibility alias
run_erp_worker = run_agent_server_worker


def run_kafka_worker():
    """Start Kafka consumer worker daemon in separate process."""
    import os
    os.environ["IS_ERP_SERVER_PROCESS"] = "false"
    from src.workers.kafka_consumer import run_kafka_consumer
    logger.info("Starting Kafka Consumer Worker Daemon...")
    run_kafka_consumer()


def run_slack_worker():
    """Start Slack Socket Mode listener in separate process."""
    import os
    os.environ["IS_ERP_SERVER_PROCESS"] = "false"
    from src.workers.slack_listener import start_slack_socket_listener
    logger.info("Starting Slack Socket Mode Listener Daemon...")
    start_slack_socket_listener()


def start_all_workers():
    """Start Agent Traceability & ERP Hub, Kafka consumer, and Slack Socket Mode listener concurrently."""
    logger.info("==================================================================")
    logger.info("🚀 Starting Autonomous Procurement Multi-Agent System")
    logger.info("   • Real-Time Agent Traceability & Embedded ERP: http://localhost:8002")
    logger.info("   • Kafka Consumer Broker: localhost:9092 (topic: inventory-events)")
    logger.info("   • State Checkpointer: PostgreSQL localhost:5432")
    logger.info("   • Slack Human-in-the-Loop: Socket Mode (Zero Ingress)")
    logger.info("==================================================================")

    p_server = multiprocessing.Process(target=run_agent_server_worker, name="AgentServerWorker")
    p_kafka = multiprocessing.Process(target=run_kafka_worker, name="KafkaConsumerWorker")
    p_slack = multiprocessing.Process(target=run_slack_worker, name="SlackListenerWorker")

    processes = [p_server, p_kafka, p_slack]

    for p in processes:
        p.daemon = True
        p.start()

    def shutdown(sig, frame):
        logger.info("\nReceived stop signal %s. Gracefully terminating all agent workers...", sig)
        for p in processes:
            if p.is_alive():
                p.terminate()
        for p in processes:
            p.join(timeout=3)
        logger.info("All agent processes stopped cleanly.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    target_map = {
        "AgentServerWorker": run_agent_server_worker,
        "MockERPServerWorker": run_agent_server_worker,
        "KafkaConsumerWorker": run_kafka_worker,
        "SlackListenerWorker": run_slack_worker,
    }

    try:
        while True:
            time.sleep(2)
            for i, p in enumerate(processes):
                if not p.is_alive():
                    logger.warning("Worker %s stopped (exit code %s). Automatically restarting...", p.name, p.exitcode)
                    target = target_map.get(p.name, run_agent_server_worker)
                    new_p = multiprocessing.Process(target=target, name=p.name)
                    new_p.daemon = True
                    new_p.start()
                    processes[i] = new_p
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    start_all_workers()
