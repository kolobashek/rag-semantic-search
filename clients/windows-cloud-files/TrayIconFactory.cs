using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Runtime.InteropServices;

namespace RagCloudFiles;

internal static partial class TrayIconFactory
{
    public static Icon Create(Icon baseIcon, ClientRunState state)
    {
        using Bitmap bitmap = new(32, 32, PixelFormat.Format32bppArgb);
        using (Graphics graphics = Graphics.FromImage(bitmap))
        {
            graphics.Clear(Color.Transparent);
            graphics.CompositingQuality = CompositingQuality.HighQuality;
            graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
            graphics.SmoothingMode = SmoothingMode.AntiAlias;
            graphics.DrawIcon(baseIcon, new Rectangle(0, 0, 32, 32));

            Color statusColor = state switch
            {
                ClientRunState.Syncing => Color.FromArgb(42, 132, 224),
                ClientRunState.UpToDate => Color.FromArgb(38, 166, 91),
                ClientRunState.Offline => Color.FromArgb(239, 152, 38),
                ClientRunState.Error => Color.FromArgb(218, 68, 68),
                _ => Color.FromArgb(125, 135, 148),
            };
            Rectangle indicator = new(19, 19, 12, 12);
            using Pen border = new(Color.White, 2F);
            using SolidBrush fill = new(statusColor);
            graphics.FillEllipse(fill, indicator);
            graphics.DrawEllipse(border, indicator);
        }

        nint handle = bitmap.GetHicon();
        try
        {
            using Icon borrowed = Icon.FromHandle(handle);
            return (Icon)borrowed.Clone();
        }
        finally
        {
            DestroyIcon(handle);
        }
    }

    [LibraryImport("user32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool DestroyIcon(nint icon);
}
