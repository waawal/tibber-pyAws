set -e
flake8 tibber_aws/
pylint --disable=C,R,no-member,no-name-in-module  tibber_aws/
