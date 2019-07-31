from setuptools import setup

setup(
    name='tibber_aws',
    packages=['tibber_aws'],
    install_requires=['boto3'],
    version='0.1.1',
    description='A python3 library to communicate with Aws',
    python_requires='>=3.5.3',
    author='Tibber',
    author_email='hello@tibber.com',
    url='https://github.com/tibbercom/tibber-pyAws',
    license="MIT",
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules'
        ]
)
