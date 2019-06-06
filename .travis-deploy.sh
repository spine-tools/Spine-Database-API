#!/bin/sh
set -e

# setup ssh-agent and provide the GitHub deploy key
#eval "$(ssh-agent -s)"
#openssl aes-256-cbc -K $encrypted_151fbad3b0ea_key -iv $encrypted_151fbad3b0ea_iv -in deploy-key.enc -out deploy-key -d
#chmod 600 deploy-key
#ssh-add deploy-key
#
pwd
# commit the assets in docs/build/ to the gh-pages branch and push to GitHub using SSH
./node_modules/.bin/gh-pages -d docs/build/ -b gh-pages -r git@github.com:${TRAVIS_REPO_SLUG}.git
