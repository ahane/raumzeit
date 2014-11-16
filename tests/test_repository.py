from raumzeit.repo import Repository
import pytest

def test_signatures_and_collection_add():
    r = Repository()
    

    with pytest.raises(NotImplementedError):
        r.iter_happenings()

    with pytest.raises(NotImplementedError):
        r.iter_happenings_timespan(None, None)

    with pytest.raises(NotImplementedError):
        r.iter_locations()

    with pytest.raises(NotImplementedError):
        r.iter_locations_join(None)

    with pytest.raises(NotImplementedError):    
        r.iter_persons()
    
    with pytest.raises(NotImplementedError):        
        r.iter_persons_join(None)