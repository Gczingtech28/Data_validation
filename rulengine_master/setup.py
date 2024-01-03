import setuptools

setuptools.setup(
    name="Validations",
    version="1.1.1",
    author="Zing Mind",
    author_email="abc@zingmind.com",
    description="Validations",
    setup_requires=["wheel"],
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=['pyodbc','jsonschema']
)

