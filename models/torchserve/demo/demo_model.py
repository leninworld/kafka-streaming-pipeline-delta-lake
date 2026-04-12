import torch
from torch import nn


class DemoMathModel(nn.Module):
    """A tiny model used only to demonstrate TorchServe wiring."""

    def forward(self, x):
        return x * 2.0 + 1.0
