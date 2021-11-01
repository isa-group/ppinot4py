import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ppinot4py", 
    version="1.2",
    author="ISA Group",
    author_email="resinas@us.es",
    description="PPINOT for Python (ppinot4py)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/isa-group/ppinot4py",
    packages=setuptools.find_packages(),
    license='GPL 3.0',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'numpy',
        'business-duration'
    ]
)