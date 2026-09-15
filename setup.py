from setuptools import setup, find_packages


setup(name="tap-notion",
      version="0.0.5",
      description="Singer.io tap for extracting data from Notion API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_notion"],
      install_requires=[
        "singer-python==6.8.0",
        "requests==2.34.2",
        "backoff==2.2.1"
      ],
      extras_require={"dev": ["pylint", "ipdb", "pytest"]},
      entry_points="""
          [console_scripts]
          tap-notion=tap_notion:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_notion": ["schemas/*.json"],
      },
      include_package_data=True,
)
