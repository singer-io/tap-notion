from setuptools import setup

setup(name="tap-notion",
      version="1.0.0",
      description="Singer.io tap for extracting data from Notion API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_notion"],
      install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
   
      ],
      entry_points="""
          [console_scripts]
          tap-notion=tap_notion:main
      """,
      packages=["tap_notion"],
      package_data = {
          "tap_notion": ["schemas/*.json"],
      },
      include_package_data=True,
)