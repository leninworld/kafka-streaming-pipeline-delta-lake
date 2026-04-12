import json

import torch
from ts.torch_handler.base_handler import BaseHandler


class DemoMathHandler(BaseHandler):
    """
    Beginner-friendly TorchServe handler.

    It accepts numbers and returns model outputs where:
    output = input * 2 + 1
    """

    def preprocess(self, data):
        # TorchServe may send the request body in either "data" or "body".
        payload = data[0].get("data") or data[0].get("body")

        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")

        if isinstance(payload, str):
            payload = json.loads(payload)

        values = payload.get("instances", payload)

        # Convert user input into a float tensor the model can consume.
        return torch.tensor(values, dtype=torch.float32)

    def inference(self, inputs):
        with torch.no_grad():
            return self.model(inputs)

    def postprocess(self, inference_output):
        # Convert the tensor back into plain Python numbers for JSON output.
        return [{"predictions": inference_output.tolist()}]
