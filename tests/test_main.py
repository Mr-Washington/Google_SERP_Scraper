#from .context import new_slug

import main

def test_main(capsys, example_fixture):
    main.new_entry({})
    # pylint: disable=W0612,W0613
