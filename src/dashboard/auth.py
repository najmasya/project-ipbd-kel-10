import os
import streamlit as st
import streamlit_authenticator as stauth

AUTH_USERNAME = os.getenv("STREAMLIT_AUTH_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("STREAMLIT_AUTH_PASSWORD", "admin123")


def get_authenticator():
    credentials = {
        "usernames": {
            AUTH_USERNAME: {
                "email": f"{AUTH_USERNAME}@example.com",
                "name": AUTH_USERNAME,
                "password": AUTH_PASSWORD,
            }
        }
    }
    authenticator = stauth.Authenticate(
        credentials,
        "ipbd_auth",
        "ipbd_dashboard_cookie",
        1,
    )
    return authenticator


def require_auth():
    if "authenticator" not in st.session_state:
        st.session_state["authenticator"] = get_authenticator()

    authenticator = st.session_state["authenticator"]
    authenticator.login()

    if st.session_state.get("authentication_status") is False:
        st.error("Username/password salah")
        st.stop()

    if st.session_state.get("authentication_status") is None:
        st.warning("Silakan login untuk melanjutkan")
        st.stop()

    name = st.session_state.get("name", "admin")
    username = st.session_state.get("username", AUTH_USERNAME)

    authenticator.logout("Logout", "sidebar")
    st.sidebar.success(f"Selamat datang, {name}")
    return name, username
