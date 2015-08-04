test:
	python setup.py test

release:
	python setup.py register -r pypi
	python setup.py sdist upload -r pypi
