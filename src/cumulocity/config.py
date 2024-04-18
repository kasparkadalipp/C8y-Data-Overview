import base64
import os

from c8y_api import CumulocityApi


def _getEnv(key: str):
    value = os.getenv(key)
    if value is None or not value:
        raise EnvironmentError(f"Missing or empty environment variable: {key}")
    return value


def getCumulocityApi():
    base_url = _getEnv("C8Y_TENANT_URL")
    tenant_id = _getEnv("C8Y_TENANT_ID")
    username = _getEnv("C8Y_USERNAME")
    password = _getEnv("C8Y_PASSWORD")

    if not base_url.startswith("https:/"):
        base_url = "https:/" + base_url

    return CumulocityApi(
        base_url=base_url,
        tenant_id=tenant_id,
        username=username,
        password=password
    )


def getToken():
    tenant = _getEnv("C8Y_TENANT_ID")
    username = _getEnv("C8Y_USERNAME")
    password = _getEnv("C8Y_PASSWORD")

    return base64.b64encode(f"{tenant}/{username}:{password}".encode()).decode()


def getAuthHeader():
    return f"Basic {getToken()}"
