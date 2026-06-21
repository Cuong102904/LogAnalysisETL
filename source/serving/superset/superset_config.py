import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-supersecret")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db",
)
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False
ENABLE_PROXY_FIX = True
