import os
import datetime
from setuptools import find_packages
from setuptools import setup
try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements

install_requirements = parse_requirements('requirements.txt', session=False)
requirements = [str(ir.req) for ir in install_requirements]

if os.getenv('DOCKER_MODE') == True:
    print("Docker mode found")
    kwargs = dict(version='0+d'+datetime.date.today().strftime('%Y%m%d'))
else:
    kwargs = dict(
        # use_scm_version={'version_scheme': 'post-release'},
        use_scm_version=True,
        setup_requires=['setuptools_scm', 'setuptools_scm_git_archive'],
    )

setup(
    name='wt-mgr',
    # use_scm_version=True,
    description='Webex Teams Config Tool.',
    author='Federico Lovison',
    author_email='flovison@cisco.com',
    packages=find_packages("."),
    include_package_data=True,
    # setup_requires=['setuptools_scm', 'setuptools_scm_git_archive'],
    install_requires=requirements,
    # extra_require={
    #         ':python_version<"3.2"': [
    #             'functools32==3.2.3.post2'
    #         ]
    #     },
    entry_points='''
        [console_scripts]
        wt-mgr=wt_mgr.wt_mgr:main
    ''',
    **kwargs
)
