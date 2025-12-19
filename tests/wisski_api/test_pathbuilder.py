import pytest

from wisski.pathbuilder import Path, Pathbuilder
from wisski.field import Field


@pytest.fixture
def pathbuilder():
    pb = Pathbuilder("test", "Test", "test_adapter")
    yield pb  # this is what the test will receive


@pytest.fixture
def paths():
    paths = [
        Path("person", "Person", Field("person_field", None), ["http://test.com/Person"], type: Path.Type.GROUP,
        Path("person", "Person", Field("person_field", None), ["http://test.com/Person"], type: Type.GROUP,
        Path("person", "Person", Field("person_field", None), ["http://test.com/Person"], type: Type.GROUP,
        Path("person", "Person", Field("person_field", None), ["http://test.com/Person"], type: Type.GROUP,
    ]
    yield path


def test_add_empty_path():

    test_path = Path("test_path", "Test Path", test_field, [])

    print("after?", test_path)

    pb.add_path(test_path)
