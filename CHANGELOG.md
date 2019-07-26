# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]


## [v1.0-alpha.3] - 2019-07-26

* Set hbase.rootDir in hbase docker image

* Add volumes for hbase state to dockerfile

## [v1.0-alpha.2] - 2019-07-24

* Fix hbase version suffix in the travis build script.


## [v1.0-alpha.1] - 2019-07-24

* Uplift HBase to 2.1.5 and refactor code accordingly

* Uplift Kafka to 2.1.1 and refactor code accordingly

* Fix creation of DB session for cached lookups

* Change purge retention logic to allow for FOREVER interval.

* Fix best fit interval sellection to allow for FOREVER interval.

* Optimise some high use code following profiling


## [v0.6.0-alpha.6] - 2019-03-13

* Change default admin path to /statsAdmin

* Uplift stroom-auth library to v1.0-beta.8

* Uplift stroom-query library to v2.1-beta.21


## [v0.6.0-alpha.5] - 2019-02-20

[Unreleased]: https://github.com/gchq/stroom-stats/compare/v1.0-alpha.3...master
[v1.0-alpha.3]: https://github.com/gchq/stroom-stats/compare/v1.0-alpha.2...v1.0-alpha.3
[v1.0-alpha.2]: https://github.com/gchq/stroom-stats/compare/v1.0-alpha.1...v1.0-alpha.2
[v1.0-alpha.1]: https://github.com/gchq/stroom-stats/compare/v0.6.0-alpha.6...v1.0-alpha.1
[v0.6.0-alpha.6]: https://github.com/gchq/stroom-stats/compare/v0.6.0-alpha.5...v0.6.0-alpha.6
[v0.6.0-alpha.5]: https://github.com/gchq/stroom-stats/compare/v0.6.0-alpha.4...v0.6.0-alpha.5
