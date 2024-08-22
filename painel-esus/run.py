from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import dotenv_values
from src.main.composers.schedule_compose import generate_base_scheduled
from src.main.server.server import app

if __name__ == "__main__":
    config = dotenv_values(".env")

    if not app.debug:
        scheduler = BackgroundScheduler()
        scheduler.start()
        generate_base_scheduled(scheduler)

    if "ssl_cert" in config and 'ssl_key' in config:
        app.run(host="0.0.0.0", port=5001, debug=False,
                ssl_context=(config['ssl_cert'], config['ssl_key']))
    else:
        app.run(host="0.0.0.0", port=5001, debug=False)
