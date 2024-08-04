from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigatewayv2 as apigatewayv2,
    aws_apigatewayv2_integrations as integrations,
    CfnOutput,
)
from constructs import Construct

class LambdaFunctionTestingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Lambda Function
        webhook_lambda_function = _lambda.Function(
            self, "WebhookLambdaFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler='lambda_function.lambda_handler',
            code=_lambda.Code.from_asset('lambda_function')
        )
        
        # HTTP API Gateway
        api = apigatewayv2.HttpApi(
            self, "WebHook",
            api_name="WebHook",
            create_default_stage=True
        )

        # POST route to the API Gateway
        api.add_routes(
            path='/post',
            methods=[apigatewayv2.HttpMethod.POST],
            integration=integrations.HttpLambdaIntegration('WebhookLambdaIntegration', webhook_lambda_function)
        )

        # Output API endpoint url
        CfnOutput(
            self, "APIEndpoint",
            value=api.url or "Something went wrong with the deployment"
        )