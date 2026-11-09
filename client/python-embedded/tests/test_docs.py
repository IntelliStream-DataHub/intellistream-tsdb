# SPDX-License-Identifier: AGPL-3.0-only
"""Every public name has a docstring, the type stub carries the same ones (so
IDEs show what help() shows), and the module's example runs."""

import ast
import doctest
import inspect
import pathlib

import tsdb_embedded

STUB = pathlib.Path(__file__).resolve().parents[1] / "tsdb_embedded.pyi"


def public(obj):
    return {n: getattr(obj, n) for n in dir(obj) if not n.startswith("_")}


def stub_docs():
    """{"Client.query_group": docstring, ...} from the stub, and the module's under ""."""
    tree = ast.parse(STUB.read_text())
    docs = {"": ast.get_docstring(tree)}
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            docs[node.name] = ast.get_docstring(node)
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                # sequence operators (__len__, __getitem__, +) are typing only: Python documents them
                if isinstance(item, ast.FunctionDef) and item.name not in (
                    "__len__", "__getitem__", "__add__", "__radd__", "__getattr__"
                ):
                    docs[f"{node.name}.{item.name}"] = ast.get_docstring(item)
    return docs


def runtime_docs():
    docs = {"": tsdb_embedded.__doc__}
    for name, obj in public(tsdb_embedded).items():
        if callable(obj):
            docs[name] = obj.__doc__
        if inspect.isclass(obj) and obj.__module__ == "tsdb_embedded":
            for attr, member in public(obj).items():
                if not (inspect.isclass(obj) and issubclass(obj, BaseException)):
                    docs[f"{name}.{attr}"] = member.__doc__
            for dunder in ("__enter__", "__exit__"):
                if dunder in obj.__dict__:
                    docs[f"{name}.{dunder}"] = obj.__dict__[dunder].__doc__
    return {k: inspect.cleandoc(v) if v else None for k, v in docs.items()}


def test_everything_is_documented():
    missing = [name or "the module" for name, doc in runtime_docs().items() if not doc]
    assert not missing, f"no docstring: {missing}"


def test_the_stub_has_the_same_docstrings():
    stub, runtime = stub_docs(), runtime_docs()
    stub.pop("Db.__new__")  # documented on the class
    assert sorted(stub) == sorted(runtime), "the stub and the module name different things"
    differ = [name for name in runtime if stub[name] != runtime[name]]
    assert not differ, f"docstrings differ between tsdb_embedded.pyi and the module: {differ}"


def test_module_example_runs(tmp_path):
    test = doctest.DocTestParser().get_doctest(
        tsdb_embedded.__doc__.replace("/tmp/tsdb-embedded", str(tmp_path / "db")), {}, "tsdb_embedded", None, 0
    )
    runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS)
    runner.run(test)
    assert runner.summarize(verbose=False).failed == 0
