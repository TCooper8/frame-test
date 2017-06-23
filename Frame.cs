using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FrameTest
{
    public enum FrameColumn
    {
        CatergoricalColumn = 0,
        NumericColumn = 1,
        BinaryColumn = 2,
    }

    public interface FrameValue
    {
        FrameColumn Tag { get; }
    }

    public struct Number: FrameValue
    {
        public readonly double Value;

        public Number(double value)
        {
            this.Value = value;
        }

        FrameColumn FrameValue.Tag
        {
            get
            {
                return FrameColumn.NumericColumn;
            }
        }
    }

    public struct Binary: FrameValue
    {
        public readonly bool Value;

        public Binary(bool value)
        {
            this.Value = value;
        }

        FrameColumn FrameValue.Tag
        {
            get
            {
                return FrameColumn.BinaryColumn;
            }
        }
    }

    public struct Category: FrameValue
    {
        public readonly string Value;

        public Category(string value)
        {
            this.Value = value;
        }

        FrameColumn FrameValue.Tag
        {
            get
            {
                return FrameColumn.CatergoricalColumn;
            }
        }
    }

    class InferInfo
    {
        public bool IsNumeric;
        public bool IsContinuous;
        public HashSet<string> UniqueValues;
        public Int64 NumCount;
        public Int64 ContCount;
        public Int64 Count;
    }

    public struct Frame
    {
        public readonly FileInfo File;
        public readonly string[] ColumnKeys;
        public readonly FrameColumn[] Schema;
        public readonly Int64 Count;

        public Frame(
            FileInfo file,
            string[] columnKeys,
            FrameColumn[] schema,
            Int64 count
            )
        {
            this.File = file;
            this.ColumnKeys = columnKeys;
            this.Schema = schema;
            this.Count = count;
        }

        public static IEnumerable<FrameValue[]> rows(Frame frame)
        {
            using (var reader = new BinaryReader(frame.File.OpenRead()))
            {
                for (var i = 0; i < frame.Count; i++)
                {
                    var array = frame.Schema.Select<FrameColumn, FrameValue>(columnType =>
                    {
                        switch (columnType)
                        {
                            case FrameColumn.NumericColumn:
                                return new Number(reader.ReadDouble());

                            case FrameColumn.CatergoricalColumn:
                                return new Category(reader.ReadString());

                            case FrameColumn.BinaryColumn:
                                return new Binary(reader.ReadBoolean());

                            default:
                                return null;
                        }
                    });
                    yield return array.ToArray();
                }
            }
        }

        public static async Task<Frame> ofCsv(FileInfo src, FileInfo dst, FrameColumn[] schema)
        {
            using (var writer = new BinaryWriter(dst.OpenWrite()))
            using (var reader = src.OpenText())
            {
                var columnKeysLine = await reader.ReadLineAsync();
                var columnKeys = columnKeysLine.Split(',').Select(part => part.Trim('"')).ToArray();
                var count = 0L;

                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();
                    var pairs = line.Split(',').Select(part => part.Trim('"')).Zip(schema, (a, b) => new Tuple<string, FrameColumn>(a, b));
                    count += 1;
                    if (count % 10000 == 0L) Console.WriteLine("ofCsv {0}", count);

                    foreach (var pair in pairs)
                    {
                        var column = pair.Item1;
                        var columnType = pair.Item2;

                        switch (columnType)
                        {
                            case FrameColumn.NumericColumn:
                                double value = Double.NaN;
                                Double.TryParse(column, out value);
                                writer.Write(value);
                                break;

                            case FrameColumn.CatergoricalColumn:
                                writer.Write(column);
                                break;
                            
                            default:
                                break;
                        }
                    }
                }

                return new Frame(dst, columnKeys, schema, count);
            }
        }

        public static FrameColumn[] infer(Frame frame)
        {
            var infos = frame.Schema.Select<FrameColumn, InferInfo>(frameColumn =>
            {
                var info = new InferInfo();
                info.UniqueValues = new HashSet<string>();
                info.IsContinuous = true;
                info.IsNumeric = true;

                return info;
            }).ToArray();

            var count = 0L;
            foreach (var row in Frame.rows(frame))
            {
                count += 1;
                if (count % 10000 == 0L) Console.WriteLine("infer {0}", count);
                for (int i = 0; i < row.Length; i++)
                {
                    var info = infos[i];
                    var value = row[i];

                    if (value.Tag == FrameColumn.CatergoricalColumn)
                    {
                        var str = ((Category)value).Value;
                        double cont = Double.NaN;

                        var isNumeric = Double.TryParse(str, out cont);
                        var isCont = Math.Round(cont) == cont;
                        info.ContCount += (isNumeric && isCont) ? 1 : 0;
                        info.NumCount += isNumeric ? 1 : 0;
                        //info.UniqueValues.Add(str);
                        info.Count += 1;
                    }
                }
            }

            return infos.Select(info =>
            {
                if (info.ContCount == info.Count) return FrameColumn.NumericColumn;
                else if (info.NumCount == info.Count)
                {
                    if (info.ContCount < 30) return FrameColumn.CatergoricalColumn;
                    else return FrameColumn.NumericColumn;
                }
                else return FrameColumn.CatergoricalColumn;
            }).ToArray();
        }
    }
}
