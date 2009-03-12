
# this is using the new method of installing the schema rather than just
# sourcing the sql
cd $MYTESTAREA/T0/operations
python installT0Schema.py
cd $PBIN
python t0astgrants.py

