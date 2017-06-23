using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FrameTest
{
    class Program
    {
        static async Task MainAsync()
        {
            Directory.CreateDirectory("C:/cache");
            var frame = await Frame.ofCsv(
                new FileInfo("C:/datasets/retention_big.csv"),
                new FileInfo("C:/cache/tmp.frame"),
                new FrameColumn[]
                {
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                    FrameColumn.CatergoricalColumn,
                }
            );
            Console.WriteLine("Frame {0}", frame);
            var schema = Frame.infer(frame);
        }

        static void Main(string[] args)
        {
            var watch = new Stopwatch();
            watch.Start();
            Task.WaitAll(MainAsync());
            watch.Stop();
            Console.WriteLine("Done in {0}", watch.Elapsed.TotalMinutes);
        }
    }
}
