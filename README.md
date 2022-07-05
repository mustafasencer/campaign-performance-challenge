# Campaign Performance Analysis

<a href="https://codecov.io/gh/mustafasencer/aklamio-challenge" target="_blank">
    <img src="https://img.shields.io/codecov/c/github/mustafasencer/aklamio-challenge?color=%2334D058" alt="Coverage">
</a>

<a href="https://github.com/mustafasencer/aklamio-challenge/actions?query=workflow%3ATest" target="_blank">
    <img src="https://github.com/mustafasencer/aklamio-challenge/workflows/Test/badge.svg" alt="Test">
</a>

### 📦 Dependencies

```shell
make deps
make dev-deps
```

```shell
make run-infra-background
```

> ❗ `docker` and `docker-compose` should be installed on the host machine.

### 💻 Migration

```shell
make migrate-scenario-(1st|2nd)
```

### 💨 Run

```shell
make run-scenario-(1st|2nd)
```

### 💄 Format and Lint

```shell
make format
make lint
```

### 🚨 Tests

```shell
make test
# or
make test-with-coverage
```

### 📝 Docs

#### 1st Scenario

###### Assumptions:

* Data deduplication decision has been taken based on the data analysis done
  in [a link](https://github.com/user/repo/blob/branch/generate_report.py).
* The decision of keeping the last duplicate event has been taken.
* `click_through_rate` has been calculated based on a **30 min** time window check where
  the presence of a `ReferralPageLoad` event is checked before firing of the `ReferralRecommendClick` event.
    * Belonging to the same customer and user.

#### 2nd Scenario

###### Assumptions:

* Data deduplication has been discarded and consequently each event has been processed.
* The same `click_through_rate` assumptions has been applied.

> 📝 The presence of various duplicate events and the way they are treated by the 2 scenarios has resulted in different numbers for ``page_loads`` numbers.
