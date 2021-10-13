from examples import get_flows_by_sector, write_bibliography, get_flows_by_activity


def test_write_bibliography():
    write_bibliography.main()


def test_get_flows_by_sector():
    get_flows_by_sector.main()


def test_get_flows_by_activity():
    get_flows_by_activity.main()
