import os
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

AUTH_USERNAME = os.getenv("STREAMLIT_AUTH_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("STREAMLIT_AUTH_PASSWORD", "admin123")


def get_authenticator():
    config = {
        "credentials": {
            "usernames": {
                AUTH_USERNAME: {
                    "email": f"{AUTH_USERNAME}@example.com",
                    "name": AUTH_USERNAME,
                    "password": AUTH_PASSWORD,
                }
            }
        },
        "cookie": {
            "expiry_days": 1,
            "key": "ipbd_dashboard_cookie",
            "name": "ipbd_auth",
        },
        "preauthorized": {"emails": []},
    }
    authenticator = stauth.Authenticate(
        config["credentials"],
        config["cookie"]["name"],
        config["cookie"]["key"],
        config["cookie"]["expiry_days"],
    )
    return authenticator


def require_auth():
    if "authenticator" not in st.session_state:
        st.session_state["authenticator"] = get_authenticator()

    authenticator = st.session_state["authenticator"]
    name, authentication_status, username = authenticator.login("Login", "main")

    if authentication_status is False:
        st.error("Username/password salah")
        st.stop()

    if authentication_status is None:
        st.warning("Silakan login untuk melanjutkan")
        st.stop()

    st.sidebar.success(f"Selamat datang, {name}")
    authenticator.logout("Logout", "sidebar")
    return name, username
