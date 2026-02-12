"""
批量将文件夹中的 AVIF 文件转换为 JPG 格式

用法示例：
    python -m scripts.avif_to_jpg --folder "D:/images"
    或
    python -m scripts.avif_to_jpg -f "D:/images"

说明：
- 依赖：pillow 和 pillow-heif，请确保已安装：
    pip install pillow pillow-heif
- 脚本会扫描指定文件夹下的所有 .avif 文件
- 转换后的 JPG 文件会保存在同一目录下，文件名与原文件相同，扩展名改为 .jpg
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List

try:
    from PIL import Image
    import pillow_heif
    # 注册 HEIF/AVIF 格式支持
    pillow_heif.register_heif_opener()
except ImportError as e:
    print("错误：缺少必要的依赖库")
    print("请运行以下命令安装：")
    print("  pip install pillow pillow-heif")
    sys.exit(1)


def find_avif_files(folder_path: str) -> List[Path]:
    """
    查找文件夹下所有的 AVIF 文件
    
    参数:
        folder_path: 文件夹路径
        
    返回:
        AVIF 文件路径列表
    """
    folder = Path(folder_path)
    if not folder.exists():
        print(f"错误：文件夹不存在：{folder_path}")
        return []
    
    if not folder.is_dir():
        print(f"错误：路径不是文件夹：{folder_path}")
        return []
    
    # 查找所有 .avif 文件（不区分大小写）
    avif_files = list(folder.glob("*.avif")) + list(folder.glob("*.AVIF"))
    return avif_files


def convert_avif_to_jpg(avif_path: Path, quality: int = 95) -> bool:
    """
    将单个 AVIF 文件转换为 JPG 格式
    
    参数:
        avif_path: AVIF 文件路径
        quality: JPG 质量（1-100，默认95）
        
    返回:
        True 表示转换成功，False 表示失败
    """
    try:
        # 打开 AVIF 文件
        with Image.open(avif_path) as img:
            # 如果是 RGBA 模式，转换为 RGB（JPG 不支持透明度）
            if img.mode in ('RGBA', 'LA', 'P'):
                # 创建白色背景
                rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                rgb_img.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                img = rgb_img
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            
            # 生成输出文件名（同一目录，扩展名改为 .jpg）
            output_path = avif_path.with_suffix('.jpg')
            
            # 保存为 JPG
            img.save(output_path, 'JPEG', quality=quality, optimize=True)
            return True
            
    except Exception as e:
        print(f"  错误：转换失败 - {e}")
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='批量将文件夹中的 AVIF 文件转换为 JPG 格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python -m scripts.avif_to_jpg --folder "D:/images"
  python -m scripts.avif_to_jpg -f "D:/images" --quality 90
        """
    )
    
    parser.add_argument(
        '-f', '--folder',
        type=str,
        required=True,
        help='包含 AVIF 文件的文件夹路径'
    )
    
    parser.add_argument(
        '-q', '--quality',
        type=int,
        default=95,
        choices=range(1, 101),
        metavar='1-100',
        help='JPG 图片质量（1-100，默认95）'
    )
    
    args = parser.parse_args()
    
    # 查找所有 AVIF 文件
    print(f"正在扫描文件夹：{args.folder}")
    avif_files = find_avif_files(args.folder)
    
    if not avif_files:
        print("未找到任何 AVIF 文件")
        return
    
    print(f"找到 {len(avif_files)} 个 AVIF 文件")
    print(f"JPG 质量设置：{args.quality}")
    print("-" * 60)
    
    # 转换每个文件
    success_count = 0
    fail_count = 0
    
    for i, avif_file in enumerate(avif_files, 1):
        print(f"[{i}/{len(avif_files)}] 正在转换：{avif_file.name}")
        
        if convert_avif_to_jpg(avif_file, quality=args.quality):
            output_file = avif_file.with_suffix('.jpg')
            print(f"  ✓ 成功：{output_file.name}")
            success_count += 1
        else:
            print(f"  ✗ 失败：{avif_file.name}")
            fail_count += 1
    
    # 输出统计信息
    print("-" * 60)
    print(f"转换完成！")
    print(f"  成功：{success_count} 个")
    print(f"  失败：{fail_count} 个")


if __name__ == "__main__":
    main()

