#!/usr/bin/env python3
import aws_cdk as cdk
from lambda_function_testing.lambda_function_testing_stack import LambdaFunctionTestingStack


app = cdk.App()
LambdaFunctionTestingStack(app, "LambdaFunctionTestingStack",)

app.synth()
