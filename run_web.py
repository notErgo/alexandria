"""
Bitcoin Miner Data Platform — Flask web server entry point.

Run: python3 run_web.py
Access: http://localhost:5004/
Override port: MINERS_PORT=5010 python3 run_web.py
"""
import os
import sys
import signal
import subprocess

# Add src/ to path so imports resolve without package installation
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from infra.logging_config import setup_logging
setup_logging()

import logging
from flask import Flask, render_template, jsonify, request, redirect

from config import FLASK_PORT, FLASK_HOST, FLASK_DEBUG, validate_companies_config

_config_errors = validate_companies_config()
if _config_errors:
    _log = logging.getLogger('miners.config')
    for _e in _config_errors:
        _log.error('companies.json: %s', _e)
    raise SystemExit('companies.json failed schema validation — fix config before starting')

from routes.data_points import bp as data_points_bp
from routes.companies import bp as companies_bp
from routes.reports import bp as reports_bp
from routes.diagnostics import bp as diagnostics_bp
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
from routes.scrape import bp as scrape_bp
from routes.regime import bp as regime_bp
from routes.explorer import bp as explorer_bp
from routes.metric_rules import bp as metric_rules_bp
from routes.pipeline import bp as pipeline_bp
from routes.qc import bp as qc_bp
from routes.crawl import bp as crawl_bp
from routes.interpret import bp as interpret_bp
from routes.suggestions import bp as suggestions_bp
log = logging.getLogger('miners.web')


def _build_fingerprint() -> dict:
    repo_path = os.path.dirname(__file__)

    def _git(*args: str) -> str:
        try:
            out = subprocess.check_output(['git', *args], cwd=repo_path, stderr=subprocess.DEVNULL)
            return out.decode('utf-8', errors='replace').strip()
        except Exception:
            return 'unknown'

    sha = _git('rev-parse', '--short', 'HEAD')
    branch = _git('rev-parse', '--abbrev-ref', 'HEAD')
    dirty_raw = _git('status', '--porcelain', '--untracked-files=no')
    return {
        'repo_path': repo_path,
        'git_sha': sha,
        'git_branch': branch,
        'dirty': bool(dirty_raw),
    }


_BUILD_FINGERPRINT = _build_fingerprint()


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
    app.register_blueprint(scrape_bp)
    app.register_blueprint(regime_bp)
    app.register_blueprint(explorer_bp)
    app.register_blueprint(metric_rules_bp)
    app.register_blueprint(pipeline_bp)
    app.register_blueprint(qc_bp)
    app.register_blueprint(crawl_bp)
    app.register_blueprint(interpret_bp)
    app.register_blueprint(suggestions_bp)

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



    @app.route('/api/build_fingerprint')
    def build_fingerprint():
        return jsonify({'success': True, 'data': _BUILD_FINGERPRINT})

    @app.route('/')
    def index():
        from app_globals import get_db
        db = get_db()
        companies = db.get_companies()
        return render_template('landing.html', companies=companies)

    @app.route('/data-explorer')
    def data_explorer_page():
        from app_globals import get_db
        db = get_db()
        companies = db.get_companies()
        return render_template('index.html', companies=companies)

    @app.route('/ops')
    def ops_page():
        from config import SOURCE_TYPE_DISPLAY
        return render_template(
            'ops.html',
            current_sector='BTC-miners',
            all_sectors=['BTC-miners'],
            source_type_display=SOURCE_TYPE_DISPLAY,
        )

    @app.route('/diagnostics')
    def diagnostics_page():
        return render_template('diagnostics.html')

    @app.route('/dashboard')
    def dashboard_page():
        from config import get_all_tickers
        return render_template('dashboard.html', all_tickers=get_all_tickers())

    @app.route('/review')
    def review_page():
        return render_template('review.html')

    @app.route('/miner-data')
    def miner_data_page():
        return redirect('/ops?tab=review', code=302)

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
    db = get_db()
    reset_count = db.reset_interrupted_scrape_jobs()
    if reset_count:
        log.info("Reset %d interrupted scrape jobs on startup", reset_count)
    recovered_pipeline_runs = db.reset_interrupted_pipeline_runs()
    if recovered_pipeline_runs:
        log.info("Recovered %d interrupted pipeline runs on startup", recovered_pipeline_runs)

    # Auto-enqueue scrape jobs for companies that have never been scraped.
    # Fires once per company lifetime (never_run status only). Subsequent
    # restarts skip already-scraped companies so this is safe to run every boot.
    _db = db
    try:
        _never_run = [
            c for c in _db.get_all_companies(active_only=True)
            if c.get('scraper_mode', 'skip') != 'skip'
            and c.get('scraper_status', 'never_run') == 'never_run'
        ]
        for _co in _never_run:
            try:
                _db.enqueue_scrape_job(_co['ticker'], 'historic')
                log.info("Auto-enqueued first scrape for %s (mode=%s)", _co['ticker'], _co['scraper_mode'])
            except ValueError:
                pass  # job already pending — ignore
        if _never_run:
            log.info("Startup: auto-enqueued %d never-run scrape jobs", len(_never_run))
    except Exception as _e:
        log.warning("Startup auto-enqueue failed (non-fatal): %s", _e)

    worker = get_scrape_worker()
    worker.start()
    log.info("ScrapeWorker started")
    app = create_app()
    log.info("Starting Miner Data Platform on port %d", FLASK_PORT)
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG, threaded=True)
