import io
from os.path import abspath, dirname, join
from setuptools import find_packages, setup


REQUIREMENTS = [
    'requests',
    'six',
    'websocket-client',
]


HERE = dirname(abspath(__file__))
LOAD_TEXT = lambda name: io.open(join(HERE, name), encoding='UTF-8').read()
DESCRIPTION = '\n\n'.join(LOAD_TEXT(_) for _ in [
    'README.rst',
    'CHANGES.rst',
])
setup(
    name='socketIO-client-2',
    version='0.7.3',
    description='A socket.io client library',
    long_description=DESCRIPTION,
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Development Status :: 5 - Production/Stable',
    ],
    keywords='socket.io node.js',
    author='John Feusi',
    author_email='feus4177@gmail.com',
    url='https://github.com/feus4177/socketIO-client-2',
    install_requires=REQUIREMENTS,
    tests_require=[
        'nose',
        'coverage',
    ],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False)
