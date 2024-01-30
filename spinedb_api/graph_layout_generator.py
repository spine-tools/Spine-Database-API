######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
This module defines the :class:`.GraphLayoutGenerator` class.
"""
import math
import numpy as np
from numpy import atleast_1d as arr
from scipy.sparse.csgraph import dijkstra


class GraphLayoutGenerator:
    """A class to build an optimised layout for an undirected graph.
    This can help visualizing the Spine data structure of multi-dimensional entities.
    """

    def __init__(
        self,
        vertex_count,
        src_inds=(),
        dst_inds=(),
        spread=0,
        heavy_positions=None,
        max_iters=12,
        weight_exp=-2,
        is_stopped=lambda: False,
        preview_available=lambda x, y: None,
        layout_available=lambda x, y: None,
        layout_progressed=lambda iter: None,
    ):
        """
        Args:
            vertex_count (int): The number of vertices in the graph. Graph vertices will have indices 0, 1, 2, ...
            src_inds (tuple, optional): The indices of the source vertices of each edge.
            dst_inds (tuple, optional): The indices of the destination vertices of each edge.
            spread (int, optional): the ideal edge length.
            heavy_positions (dict, optional): a dictionary mapping vertex indices to another dictionary
                with keys "x" and "y" specifying the position it should have in the generated layout.
            max_iters (int, optional): the maximum numbers of iterations of the layout generation algorithm.
            weight_exp (int, optional): The exponential decay rate of attraction between vertices. The higher this
                number, the lesser the attraction between distant vertices.
            is_stopped (function, optional): A function to call without arguments, that returns a boolean indicating
                whether the layout generation process needs to be stopped.
            preview_available (function, optional): A function to call after every iteration with two lists, x and y,
                representing the current layout.
            layout_available (function, optional): A function to call after the last iteration with two lists, x and y,
                representing the final layout.
            layout_progressed (function, optional): A function to call after each iteration with the current iteration
                number.
        """
        super().__init__()
        if vertex_count == 0:
            vertex_count = 1
        if heavy_positions is None:
            heavy_positions = dict()
        self.vertex_count = vertex_count
        self.src_inds = src_inds
        self.dst_inds = dst_inds
        self.spread = spread
        self.heavy_positions = heavy_positions
        self.max_iters = max(3, round(max_iters * (1 - len(heavy_positions) / self.vertex_count)))
        self.weight_exp = weight_exp
        self.initial_diameter = (self.vertex_count ** (0.5)) * self.spread
        self._is_stopped = is_stopped
        self._preview_available = preview_available
        self._layout_available = layout_available
        self._layout_progressed = layout_progressed

    def shortest_path_matrix(self):
        if not self.src_inds:
            # Graph with no edges, just vertices. Introduce fake pair of edges to help 'spreadness'.
            self.src_inds = [self.vertex_count, self.vertex_count]
            self.dst_inds = [np.random.randint(0, self.vertex_count), np.random.randint(0, self.vertex_count)]
            self.vertex_count += 1
        dist = np.zeros((self.vertex_count, self.vertex_count))
        src_inds = arr(self.src_inds)
        dst_inds = arr(self.dst_inds)
        try:
            dist[src_inds, dst_inds] = dist[dst_inds, src_inds] = self.spread
        except IndexError:
            pass
        start = 0
        slices = []
        while start < self.vertex_count:
            if self._is_stopped():
                return None
            stop = min(self.vertex_count, start + math.ceil(self.vertex_count / 10))
            slice_ = dijkstra(dist, directed=False, indices=range(start, stop))
            slices.append(slice_)
            start = stop
        matrix = np.vstack(slices)
        # Remove infinites and zeros
        matrix[matrix == np.inf] = self.spread * self.vertex_count ** (0.5)
        matrix[matrix == 0] = self.spread * 1e-6
        return matrix

    def sets(self):
        sets = []
        for n in range(1, self.vertex_count):
            pairs = np.zeros((self.vertex_count - n, 2), int)  # pairs on diagonal n
            pairs[:, 0] = np.arange(self.vertex_count - n)
            pairs[:, 1] = pairs[:, 0] + n
            mask = np.mod(range(self.vertex_count - n), 2 * n) < n
            s1 = pairs[mask]
            s2 = pairs[~mask]
            if s1.any():
                sets.append(s1)
            if s2.any():
                sets.append(s2)
        return sets

    def compute_layout(self):
        """Computes the layout using VSGD-MS and returns x and y coordinates for each vertex in the graph.

        Returns:
            tuple(list,list): x and y coordinates
        """
        if len(self.heavy_positions) == self.vertex_count:
            x, y = zip(*[(pos["x"], pos["y"]) for pos in self.heavy_positions.values()])
            self._layout_available(x, y)
            return x, y
        if self.vertex_count <= 1:
            x, y = np.array([0.0]), np.array([0.0])
            self._layout_available(x, y)
            return x, y
        matrix = self.shortest_path_matrix()
        self._layout_progressed(1)
        if matrix is None:
            return [], []
        mask = np.ones((self.vertex_count, self.vertex_count)) == 1 - np.tril(
            np.ones((self.vertex_count, self.vertex_count))
        )  # Upper triangular except diagonal
        np.random.seed(0)
        layout = np.random.rand(self.vertex_count, 2) * self.initial_diameter - self.initial_diameter / 2
        heavy_ind_list = list()
        heavy_pos_list = list()
        for ind, pos in self.heavy_positions.items():
            heavy_ind_list.append(ind)
            heavy_pos_list.append([pos["x"], pos["y"]])
        heavy_ind = arr(heavy_ind_list)
        heavy_pos = arr(heavy_pos_list)
        if heavy_ind.any():
            layout[heavy_ind, :] = heavy_pos
        weights = matrix ** self.weight_exp  # bus-pair weights (lower for distant buses)
        maxstep = 1 / np.min(weights[mask])
        minstep = 1 / np.max(weights[mask])
        lambda_ = np.log(minstep / maxstep) / (self.max_iters - 1)  # exponential decay of allowed adjustment
        sets = self.sets()  # construct sets of bus pairs
        self._layout_progressed(2)
        for iteration in range(self.max_iters):
            if self._is_stopped():
                break
            x, y = layout[:, 0], layout[:, 1]
            self._preview_available(x, y)
            self._layout_progressed(3 + iteration)
            # FIXME
            step = maxstep * np.exp(lambda_ * iteration)  # how big adjustments are allowed?
            rand_order = np.random.permutation(
                self.vertex_count
            )  # we don't want to use the same pair order each iteration
            for s in sets:
                v1, v2 = rand_order[s[:, 0]], rand_order[s[:, 1]]  # arrays of vertex1 and vertex2
                # current distance (possibly accounting for system rescaling)
                dist = ((layout[v1, 0] - layout[v2, 0]) ** 2 + (layout[v1, 1] - layout[v2, 1]) ** 2) ** 0.5
                r = (matrix[v1, v2] - dist)[:, None] * (layout[v1] - layout[v2]) / dist[:, None] / 2  # desired change
                dx1 = r * np.minimum(1, weights[v1, v2] * step)[:, None]
                dx2 = -dx1
                layout[v1, :] += dx1  # update position
                layout[v2, :] += dx2
                if heavy_ind.any():
                    layout[heavy_ind, :] = heavy_pos
        x, y = layout[:, 0], layout[:, 1]
        self._layout_available(x, y)
        return x, y
