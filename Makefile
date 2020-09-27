.PHONY: clean test release
default:
	@echo "Please choose one of the following target: dep, clean, test"
	@exit 2

dep: clean
	virtualenv --python=python3.8 ./venv
	./venv/bin/python setup.py install
	./venv/bin/pip freeze > requirements.txt
	./venv/bin/pip install -r requirements-dev.txt

clean:
	rm -rf ./build
	rm -rf ./dist
	rm -rf ./venv
	rm -rf *.egg-info

release:
	git tag `sed -n -e "s/version[\s\t]*=[\s\t]*'\([0-9.]\+\)',/\1/p" setup.py`

test:
	./venv/bin/python -m unittest -v