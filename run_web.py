"""
Bitcoin Miner Data Platform — Flask web server entry point.

Run: python3 run_web.py
Access: http://localhost:5004/
Override port: MINERS_PORT=5010 python3 run_web.py
"""
import os
import sys
import signal

# Add src/ to path so imports resolve without package installation
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from infra.logging_config import setup_logging
setup_logging()

import logging
from flask import Flask, render_template, jsonify, request

from config import FLASK_PORT
from routes.data_points import bp as data_points_bp
from routes.companies import bp as companies_bp
from routes.reports import bp as reports_bp
from routes.diagnostics import bp as diagnostics_bp
from routes.patterns import bp as patterns_bp
from routes.timeseries import bp as timeseries_bp
from routes.themes import bp as themes_bp
from routes.miner import bp as miner_bp
from routes.review import bp as review_bp
from routes.facilities import bp as facilities_bp
from routes.llm_prompts import bp as llm_prompts_bp
from routes.config import bp as config_bp
from routes.dashboard import bp as dashboard_bp
from routes.coverage import bp as coverage_bp
from routes.operations import bp as operations_bp
from routes.benchmark import bp as benchmark_bp

log = logging.getLogger('miners.web')


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
        static_folder=os.path.join(os.path.dirname(__file__), 'static'),
    )
    app.config['SECRET_KEY'] = os.environ.get('MINER_SECRET_KEY', 'dev-key-not-for-production')

    # Register blueprints
    app.register_blueprint(data_points_bp)
    app.register_blueprint(companies_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(diagnostics_bp)
    app.register_blueprint(patterns_bp)
    app.register_blueprint(timeseries_bp)
    app.register_blueprint(themes_bp)
    app.register_blueprint(miner_bp)
    app.register_blueprint(review_bp)
    app.register_blueprint(facilities_bp)
    app.register_blueprint(llm_prompts_bp)
    app.register_blueprint(config_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(coverage_bp)
    app.register_blueprint(operations_bp)
    app.register_blueprint(benchmark_bp)

    @app.errorhandler(404)
    def not_found(e):
        if request.path.startswith('/api/'):
            return jsonify({'success': False, 'error': {'message': 'Not found'}}), 404
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def server_error(e):
        log.error("500 error: %s", e, exc_info=True)
        if request.path.startswith('/api/'):
            return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
        return render_template('500.html'), 500

    @app.route('/api/status')
    def status():
        from app_globals import get_db
        db = get_db()
        companies = db.get_companies()
        data_points = db.count_data_points()
        pending_review = db.count_review_items(status='PENDING')
        return jsonify({
            'success': True,
            'data': {
                'app': 'miners',
                'version': '1.0.0',
                'companies': len(companies),
                'data_points': data_points,
                'pending_review': pending_review,
            }
        })

    @app.route('/')
    def index():
        from app_globals import get_db
        db = get_db()
        companies = db.get_companies()
        return render_template('index.html', companies=companies)

    @app.route('/patterns')
    def patterns_page():
        return render_template('patterns.html')

    @app.route('/diagnostics')
    def diagnostics_page():
        return render_template('diagnostics.html')

    @app.route('/dashboard')
    def dashboard_page():
        return render_template('dashboard.html')

    @app.route('/review')
    def review_page():
        return render_template('review.html')

    @app.route('/miner-data')
    def miner_data_page():
        return render_template('miner_data.html')

    @app.route('/company/<ticker>')
    def company_page(ticker):
        from app_globals import get_db
        db = get_db()
        company = db.get_company(ticker.upper())
        if company is None:
            return render_template('404.html'), 404
        return render_template('company.html', company=company)

    return app


def _shutdown(signum, frame):
    log.info("Shutdown signal received — stopping ScrapeWorker")
    try:
        from app_globals import get_scrape_worker
        get_scrape_worker().stop()
    except Exception:
        pass
    sys.exit(0)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

if __name__ == '__main__':
    from app_globals import get_db, get_scrape_worker
    # Reset any jobs orphaned by a previous crash before starting worker
    reset_count = get_db().reset_interrupted_scrape_jobs()
    if reset_count:
        log.info("Reset %d interrupted scrape jobs on startup", reset_count)
    worker = get_scrape_worker()
    worker.start()
    log.info("ScrapeWorker started")
    app = create_app()
    log.info("Starting Miner Data Platform on port %d", FLASK_PORT)
    app.run(host='0.0.0.0', port=FLASK_PORT, debug=True, threaded=True)
