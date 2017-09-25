#!/bin/sh

http -f POST http://127.0.0.1:8087/tasks/log-level logger=stroom.stats level=DEBUG
