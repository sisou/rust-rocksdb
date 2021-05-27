.PHONY: clippy
clippy:
	cargo clippy --all --tests -- -A clippy::upper-case-acronyms -A clippy::missing_safety_doc -D warnings
