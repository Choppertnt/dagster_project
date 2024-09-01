from setuptools import find_packages, setup

setup(
    name="project",
    version='0.1.0',
    packages=find_packages(exclude=["dagster_university_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "matplotlib",
        "google-auth",
        "google-auth-oauthlib",
        "google-auth-httplib2",
        "google-api-python-client",
        "google-auth",
        "seaborn",
        "plotly",
        "sqlalchemy",
        "python-dotenv",
        "psycopg2-binary",
        "dagster-dbt",
        "dbt-postgres",
    ],
    extras_require={"dev": ["dagster-webserver","pytest"]},
)