import os
import argparse
from PIL import Image

def convert_webp_to_jpg(input_dir, output_dir, quality=95):
    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(input_dir):
        if filename.lower().endswith(".webp"):
            webp_path = os.path.join(input_dir, filename)
            jpg_name = os.path.splitext(filename)[0] + ".jpg"
            jpg_path = os.path.join(output_dir, jpg_name)

            with Image.open(webp_path) as img:
                # WebP 可能带透明通道，JPG 不支持
                img = img.convert("RGB")
                img.save(jpg_path, "JPEG", quality=quality)

            print(f"✔ {filename} -> {jpg_name}")

    print("🎉 转换完成")

def main():
    parser = argparse.ArgumentParser(
        description="Batch convert WebP images to JPG"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="输入 WebP 图片文件夹路径"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="输出 JPG 图片文件夹路径"
    )
    parser.add_argument(
        "--quality", "-q",
        type=int,
        default=95,
        help="JPG 图片质量 (1-100)，默认 95"
    )

    args = parser.parse_args()

    convert_webp_to_jpg(args.input, args.output, args.quality)

if __name__ == "__main__":
    main()
