using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal sealed class RpcMethod
{
    internal RpcMethod(MethodInfo method)
    {
        Method = method;
        Parameters = method.GetParameters();
        ResultType = method.ReturnType == typeof(Task) ? null : method.ReturnType.GetGenericArguments()[0];
        InputIndex = -1;
        OutputIndex = -1;
        PageSourceIndex = -1;
        for (var index = 0; index < Parameters.Length; index++)
        {
            var parameter = Parameters[index];
            if (parameter.ParameterType == typeof(Stream))
            {
                if (string.Equals(parameter.Name, "destination", StringComparison.Ordinal))
                    OutputIndex = index;
                else
                    InputIndex = index;
            }
            else if (parameter.ParameterType == typeof(IPageCopySource))
                PageSourceIndex = index;
        }
        if ((InputIndex >= 0 && PageSourceIndex >= 0) || (OutputIndex >= 0 && ResultType is not null) ||
            Parameters.Count(parameter => parameter.ParameterType == typeof(Stream)) > 1)
            throw new InvalidOperationException("The application contract has an unsupported streaming signature.");
        Id = method.Name + ":" + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(
            method.ReturnType.FullName + "(" + string.Join(',', Parameters.Select(parameter => parameter.Name + ":" + parameter.ParameterType.FullName)) + ")")));
    }

    internal MethodInfo Method { get; }
    internal ParameterInfo[] Parameters { get; }
    internal Type? ResultType { get; }
    internal string Id { get; }
    internal int InputIndex { get; }
    internal int OutputIndex { get; }
    internal int PageSourceIndex { get; }

    internal bool IsControlParameter(int index) =>
        Parameters[index].ParameterType != typeof(CancellationToken) && index != InputIndex &&
        index != OutputIndex && index != PageSourceIndex;
}
