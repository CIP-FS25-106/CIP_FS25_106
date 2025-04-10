from setuptools import setup, find_packages

setup(
    name="sbb_delays_dashboard",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "dash",
        "dash-bootstrap-components",
        "pandas",
        "numpy",
        "plotly",
    ],
)