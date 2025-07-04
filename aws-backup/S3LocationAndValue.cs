namespace aws_backup;

public readonly struct S3LocationAndValue<TValue>(string s3Key, TValue value)
    where TValue : notnull
{
    public string S3Key { get; } = s3Key;
    public TValue Value { get; } = value;

    public override string ToString() => $"{S3Key}={Value}";
    
    public void Deconstruct(out string key, out TValue value)
    {
        key = S3Key;
        value = Value;
    }
}