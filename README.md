# exist-expath-file-module
EXPath file module for eXist

Installation
------------
Until this is artifact is published to Maven central, you will first need to install the EXQuery's EXPath File Module. Using Git and Apache Maven, run:

```bash
$ git clone https://github.com/exquery/exquery.git
$ cd exquery/expath-file-module
$ mvn install
```

Then to build a XAR for this module run:
```bash
$ git clone https://github.com/adamretter/exist-expath-file-module.git
$ cd exist-expath-file-module
$ mvn package
```

You will then find a XAR file in the `target/` folder
