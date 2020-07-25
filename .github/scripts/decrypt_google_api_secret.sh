#!/bin/sh

# Decrypt the file
mkdir $HOME/secrets
# --batch to prevent interactive command
# --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$GOOGLE_API_SECRET_PASSPHRASE" \
--output $HOME/secrets/google-api-credentials.json ./credentials/google-api-credentials.json.gpg