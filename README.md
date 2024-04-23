# Dangermond Preserve Tools and Scripts
Script repository for conservation technology at the Dangermond Preserve

This repository is a catalogue of scripts and tools used at the Dangermond Preserve by scientists, restoration, stewardship, and conservation technologists. 

# Setting up the environment
Open the Powershell (as an admin) the directory where you cloned this repository.
You'll need to set up your own environment where you can store information such as your authentication to access various tools/sites. 
`python -m venv .env`
This creates a .env file in your local clone of the repository where you can set environment variables. It is important that you maintain compatibility with the default ArcPy environment which runs ArcGIS Pro. Otherwise you may experience issues when running or sharing Notebooks. This link contains information about which dependencies are required for running ArcGIS Pro.

https://developers.arcgis.com/python/guide/system-requirements/#dependencies

To activate the environment, run:
`.env/Scripts/Activate`

If you want to install additional libraries, use:
`python -m pip install PACKAGENAME`
