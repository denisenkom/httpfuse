INSTALLATION
============

Create virtual environment and install required packages:

    $ virtualenv env
    $ source env/bin/activate
    $ pip install requests fusepy --use-mirrors

Modify main function in httpfuse.py to point to specific mount point and
modify directory structure (root variable)

Run httpfuse.py

    $ python httpfuse.py --conf notyetused
