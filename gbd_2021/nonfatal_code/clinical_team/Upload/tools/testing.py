class TestError(Exception):
    pass


class Test:
    """ Test is an object representing a test to be passed into a pipeline. It is created using the wrap decorator on a normal function"""

    def wrap(func):
        return Test(func)

    def __init__(self, func):
        self._test = func
        self._kargs = {}

    @property
    def test(self):
        return self._test

    @property
    def kargs(self):
        return self._kargs.copy()

    @property
    def name(self):
        return self._test.__name__

    def run(self, **kargs):
        arguments = self._kargs.copy()
        arguments.update(kargs)
        return self._test(**arguments)

    def __call__(self, **kargs):
        self._kargs = kargs
        return self

    def __repr__(self):
        return f"<{self.__class__.__name__}> {self.name}({self.kargs})"


class TestingPipeline:
    """An object that runs tests on a Pandas DataFrame"""

    def __init__(self, df, logger):
        self._df = df
        self._logger = logger
        self._results = {}

    def __call__(self, *tests):
        passed_all_tests = True
        for test in tests:
            if not isinstance(test, Test):
                TestError(
                    f"TestingPipeline only accepts Test objects but "
                    f" was called with a {type(test)}. Is the test "
                    f" wrapped?"
                )

            # Pre test logic:
            self._pre_apply_test(test)

            # Run Test
            test_passed = test.run(df=self._df)
            if not test_passed:
                passed_all_tests = False

            # Post test logic:
            self._post_apply_test(test, test_passed)
        return passed_all_tests

    def _pre_apply_test(self, test):
        pass

    def _post_apply_test(self, test, test_passed):
        self._results[test.name] = test_passed
        test_name = f"{test.name}({test.kargs})"
        self._logger.report_test(test_name, test_passed)
