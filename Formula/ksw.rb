class Ksw < Formula
  desc "AI-powered interactive Kubernetes context switcher"
  homepage "https://github.com/YonierGomez/ksw"
  url "https://github.com/YonierGomez/ksw/archive/refs/tags/v1.9.0.tar.gz"
  sha256 "136163c477f653e46c4fef76ae21f53357d06e64c9f35d05cab0927b788c471c"
  license "MIT"

  depends_on "go" => :build
  depends_on "kubernetes-cli"

  def install
    system "go", "build", "-ldflags", "-s -w", "-o", bin/"ksw", "."
  end

  test do
    assert_match "ksw v#{version}", shell_output("#{bin}/ksw -v")
  end
end
