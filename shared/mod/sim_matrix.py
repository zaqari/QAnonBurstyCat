import torch
import torch.nn as nn
import numpy as np
import pandas as pd

class probFn(nn.Module):

    def __init__(self, sigma):
        super(probFn,self).__init__()
        self.dist = torch.distributions.HalfNormal(sigma, validate_args=False)
        # self.dist = torch.distributions.HalfCauchy(sigma, validate_args=False)
        self.cos = nn.CosineSimilarity(dim=-1)

    def p(self, x):
        return torch.exp(self.dist.log_prob(x))

    def cosE(self, x, y):
        return 1. - self.cos(x.unsqueeze(1), y)

    def PROB(self, x, y):
        return self.p(self.cosE(x,y))

    def streamPROB(self, x, y):
        return torch.cat([self.p(self.cosE(i.view(1, -1), y)).view(1, -1) for i in x], dim=0)
