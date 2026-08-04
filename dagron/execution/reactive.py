"""Reactive/Observable DAG execution."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG


class ReactiveDAG:
    """Push-based reactive DAG execution system.

    Extends the incremental execution model into a reactive system.
    Setting an input value automatically cascades recomputation through
    the graph and fires subscriber callbacks when outputs change.

    Built on top of IncrementalExecutor's dirty-set and early-cutoff logic.

    Args:
        dag: The DAG defining the dependency structure.
        tasks: Dict mapping node names to callables that accept keyword
            arguments from their dependencies.

    Example::

        reactive = ReactiveDAG(dag, tasks)
        reactive.subscribe("output", lambda name, val: print(f"Updated: {val}"))
        reactive.set_input("raw_data", new_data)  # cascades automatically
    """

    def __init__(
        self,
        dag: DAG,
        tasks: dict[str, Callable[..., Any]],
    ) -> None:
        self._dag = dag
        self._tasks = dict(tasks)
        self._values: dict[str, Any] = {}
        self._subscribers: dict[str, list[Callable[[str, Any], None]]] = defaultdict(list)
        self._global_subscribers: list[Callable[[str, Any], None]] = []
        self._initialized = False

    @property
    def dag(self) -> DAG:
        """The underlying DAG."""
        return self._dag

    @property
    def values(self) -> dict[str, Any]:
        """Current values of all computed nodes (read-only copy)."""
        return dict(self._values)

    def get(self, node_name: str) -> Any:
        """Get the current value of a node.

        Args:
            node_name: Name of the node.

        Returns:
            The node's current value, or None if not yet computed.
        """
        return self._values.get(node_name)

    def subscribe(
        self,
        node_name: str,
        callback: Callable[[str, Any], None],
    ) -> Callable[[], None]:
        """Subscribe to changes on a specific node.

        Args:
            node_name: Name of the node to watch.
            callback: Function called with (node_name, new_value)
                when the node's value changes.

        Returns:
            An unsubscribe function.
        """
        self._subscribers[node_name].append(callback)

        def unsubscribe() -> None:
            self._subscribers[node_name].remove(callback)

        return unsubscribe

    def subscribe_all(
        self,
        callback: Callable[[str, Any], None],
    ) -> Callable[[], None]:
        """Subscribe to changes on any node.

        Args:
            callback: Function called with (node_name, new_value)
                when any node's value changes.

        Returns:
            An unsubscribe function.
        """
        self._global_subscribers.append(callback)

        def unsubscribe() -> None:
            self._global_subscribers.remove(callback)

        return unsubscribe

    def _notify(self, node_name: str, value: Any) -> None:
        for cb in self._subscribers.get(node_name, []):
            cb(node_name, value)
        for cb in self._global_subscribers:
            cb(node_name, value)

    def _compute_node(self, name: str) -> Any:
        """Compute a single node using current values of predecessors."""
        task_fn = self._tasks.get(name)
        if task_fn is None:
            return self._values.get(name)

        # Gather predecessor values as kwargs
        preds = self._dag.predecessors(name)
        kwargs: dict[str, Any] = {}
        for pred in preds:
            if pred.name in self._values:
                kwargs[pred.name] = self._values[pred.name]
        return task_fn(**kwargs)

    def initialize(self) -> dict[str, Any]:
        """Compute all nodes in topological order.

        This performs the initial full computation. After this,
        use ``set_input()`` for incremental updates. Any values
        set via ``set_input()`` before initialization are preserved.

        Returns:
            Dict of all computed values.
        """
        # Preserve values that were pre-set before initialization
        preset = dict(self._values)
        topo_order = [n.name for n in self._dag.topological_sort()]
        for name in topo_order:
            if name in preset:
                # Keep pre-set value, but still notify
                self._notify(name, preset[name])
            else:
                value = self._compute_node(name)
                self._values[name] = value
                self._notify(name, value)
        self._initialized = True
        return dict(self._values)

    def set_input(self, node_name: str, value: Any) -> dict[str, Any]:
        """Set an input value and cascade recomputation.

        Args:
            node_name: Name of the input node to set.
            value: New value for the node.

        Returns:
            Dict of all nodes that were recomputed, mapping name to new value.
        """
        if not self._initialized:
            self._values[node_name] = value
            return self.initialize()

        old_value = self._values.get(node_name)

        try:
            same = old_value == value
        except Exception:
            same = False

        if same:
            return {}

        self._values[node_name] = value

        # Find dirty set — all nodes that need recomputation
        dirty = set(self._dag.dirty_set([node_name]))
        dirty.discard(node_name)  # Already set manually

        # Process in topological order
        topo_order = [n.name for n in self._dag.topological_sort()]
        changed: dict[str, Any] = {node_name: value}
        self._notify(node_name, value)

        # Track which nodes actually changed (for early cutoff)
        propagation_set: set[str] = {node_name}

        for name in topo_order:
            if name == node_name or name not in dirty:
                continue

            # Check if any predecessor changed
            preds = {n.name for n in self._dag.predecessors(name)}
            if not (preds & propagation_set):
                # No predecessor changed — early cutoff
                continue

            old_val = self._values.get(name)
            new_val = self._compute_node(name)

            try:
                is_same = old_val == new_val
            except Exception:
                is_same = False

            if is_same:
                # Early cutoff — value didn't change
                continue

            self._values[name] = new_val
            propagation_set.add(name)
            changed[name] = new_val
            self._notify(name, new_val)

        return changed

    def set_inputs(self, values: dict[str, Any]) -> dict[str, Any]:
        """Set multiple input values and cascade recomputation.

        More efficient than calling set_input multiple times as it
        computes the combined dirty set.

        Args:
            values: Dict mapping node names to new values.

        Returns:
            Dict of all nodes that were recomputed.
        """
        if not self._initialized:
            self._values.update(values)
            return self.initialize()

        # Update all values first
        changed_inputs: list[str] = []
        for name, value in values.items():
            old_value = self._values.get(name)
            try:
                same = old_value == value
            except Exception:
                same = False
            if not same:
                self._values[name] = value
                changed_inputs.append(name)

        if not changed_inputs:
            return {}

        # Compute combined dirty set
        dirty = set(self._dag.dirty_set(changed_inputs))
        for name in changed_inputs:
            dirty.discard(name)

        topo_order = [n.name for n in self._dag.topological_sort()]
        changed: dict[str, Any] = {}
        propagation_set: set[str] = set(changed_inputs)

        # Notify input changes
        for name in changed_inputs:
            changed[name] = self._values[name]
            self._notify(name, self._values[name])

        for name in topo_order:
            if name in changed_inputs or name not in dirty:
                continue

            preds = {n.name for n in self._dag.predecessors(name)}
            if not (preds & propagation_set):
                continue

            old_val = self._values.get(name)
            new_val = self._compute_node(name)

            try:
                is_same = old_val == new_val
            except Exception:
                is_same = False

            if is_same:
                continue

            self._values[name] = new_val
            propagation_set.add(name)
            changed[name] = new_val
            self._notify(name, new_val)

        return changed
