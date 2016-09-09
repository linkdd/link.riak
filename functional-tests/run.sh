#!/bin/bash

aloe features/driver/scenarios.feature || exit 1
aloe features/fulltext/scenarios.feature || exit 1
