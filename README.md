# Dangermond Preserve Tools and Scripts
Script repository for conservation technology at the Dangermond Preserve

This repository is a catalogue of scripts and tools used at the Dangermond Preserve by scientists, restoration, stewardship, and conservation technologists. 

# Setting up the environment
You'll need to set up your own environment where you can store information such as your authentication to access various tools/sites. 
`pip install python-dotenv`
This creates a .env file in your local clone of the repository where you can set environment variables. 

If you want to load your stored information into a python notebook for example, run the following:
`%load_ext dotenv%`
`%dotenv`
