# The compose end-to-end checks, each one command. See README.md.

.PHONY: loghub chaos

# The loghub harness against the POC pipeline: nothing missing, unexpected or wrongly written.
loghub:
	deploy/loghub-check.sh

# The same run with the pipeline killed at 20s (back at 25s) and Dragonfly paused at 40s for 5s.
chaos:
	deploy/loghub-check.sh --chaos
