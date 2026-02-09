python - << 'EOF'
import sys
print("sys.executable:", sys.executable)
print("site-packages:", sys.path)
import pip
print("pip path:", pip.__file__)
EOF
