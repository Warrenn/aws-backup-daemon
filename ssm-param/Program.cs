// See https://aka.ms/new-console-template for more information

using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;

var path = "";
if(args.Length > 0)
    path = args[0];

if (string.IsNullOrWhiteSpace(path))
{
    Console.Error.WriteLine("Path to the parameter is required");
    return -1;
}

var ssmClient = new AmazonSimpleSystemsManagementClient();
var response = await ssmClient.GetParameterAsync(new GetParameterRequest
{
    Name = path,
    WithDecryption = true
});

Console.WriteLine(response.Parameter.Value);
return 0;