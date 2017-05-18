#include <stddef.h>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "../common.h"
#include "../strutil.h"
#include "../logging.h"
#include "../strutil.h"

using namespace std;

enum Symbol { LPAREN, RPAREN, AND, OR, THEN, FALLBACK, SET_ID };

class TreeNode {
 public:
  virtual ~TreeNode() {}

  TreeNode(Symbol symbol) : symbol_(symbol) {}

  Symbol symbol() const { return symbol_; }

  virtual std::string ToString() const = 0;

 private:
  Symbol symbol_;
};

class TerminalNode : public TreeNode {
 public:
  TerminalNode(const std::string& set_id, bool avoids)
      : TreeNode(SET_ID), set_id_(set_id), avoids_(avoids) {}

  // Identifies the graph evlements this terminal represents.
  const std::string& set_id() const { return set_id_; }

  // True if the graph elements identified by 'set_id' should be avoided. False
  // if they should be visited.
  bool avoids() const { return avoids_; }

  std::string ToString() const override {
    if (avoids_) {
      return nc::StrCat("avoid(", set_id_, ")");
    }
    return nc::StrCat("visit(", set_id_, ")");
  }

 private:
  std::string set_id_;
  bool avoids_;
};

class ExpressionNode : public TreeNode {
 public:
  ExpressionNode(Symbol symbol, std::unique_ptr<TreeNode> left,
                 std::unique_ptr<TreeNode> right)
      : TreeNode(symbol), left_(std::move(left)), right_(std::move(right)) {}

  std::string ToString() const override {
    return nc::StrCat("(", left_->ToString(), " ", std::to_string(symbol()),
                      " ", right_->ToString(), ")");
  }

 private:
  std::unique_ptr<TreeNode> left_;
  std::unique_ptr<TreeNode> right_;
};

// Splits an expression into symbols and extracts the set ids. Everything which
// is not a symbol is assumed to be a set id.
bool Tokenize(const std::string& expression, std::vector<Symbol>* symbols,
              std::vector<std::unique_ptr<TerminalNode>>* terminals) {
  std::string expression_braces_apart =
      nc::StringReplace(expression, "(", " ( ", true);
  expression_braces_apart =
      nc::StringReplace(expression_braces_apart, ")", " ) ", true);

  std::vector<std::string> split = nc::Split(expression_braces_apart, " ");
  for (size_t i = 0; i < split.size(); ++i) {
    std::string token = split[i];
    if (token == "(") {
      symbols->emplace_back(LPAREN);
    } else if (token == ")") {
      symbols->emplace_back(RPAREN);
    } else if (token == "then") {
      symbols->emplace_back(THEN);
    } else if (token == "or") {
      symbols->emplace_back(OR);
    } else if (token == "and") {
      symbols->emplace_back(AND);
    } else if (token == "visit") {
      if (i + 1 == split.size()) {
        LOG(ERROR) << "Unable to tokenize: cannot find id of elements to visit";
        return false;
      }

      symbols->emplace_back(SET_ID);
      terminals->emplace_back(nc::make_unique<TerminalNode>(split[++i], false));
    } else if (token == "avoid") {
      if (i + 1 == split.size()) {
        LOG(ERROR) << "Unable to tokenize: cannot find id of elements to avoid";
        return false;
      }

      symbols->emplace_back(SET_ID);
      terminals->emplace_back(nc::make_unique<TerminalNode>(split[++i], true));
    } else {
      LOG(ERROR) << "Unable to tokenize: unexpected token " << token;
      return false;
    }
  }

  return true;
}

// This parser parses the following grammar:
//
// Fallback    <- Fallback 'fallback' And | And
// And    <- And 'and' Or | Or
// Or     <- Or 'or' Then | Then
// Then   <- Then 'then' P | P
// P      <- '(' Then ')' | id
// id     <- 'avoid' set_name | 'visit' set_name
//
// The above grammar is left recursive and this simple descent parser cannot
// directly handle it. Here is an equivalent grammar with no left recursion that
// the parser actually parses:
//
// Fallback   <- And Fallback'
// Fallback'  <- 'fallback' And Fallback' | nil
// And   <- Or And'
// And'  <- 'and' Or And' | nil
// Or    <- Then Or'
// Or'   <- 'or' Then Or' | nil
// Then <- P Then'
// Then' <- 'then' P Then' | nil
// P <- '(' Fallback ')' | id
class Parser {
 public:
  Parser(const std::vector<Symbol>& symbols,
         std::vector<std::unique_ptr<TerminalNode>>&& terminals)
      : symbol_index_(0),
        terminal_index_(-1),
        symbols_(symbols),
        terminals_(std::move(terminals)) {
    CHECK(!symbols_.empty());
    if (symbols_[0] == SET_ID) {
      ++terminal_index_;
      CHECK(terminals_.size() > terminal_index_);
    }
  }

  std::unique_ptr<TreeNode> Parse() {
    auto to_return = Fallback();
    CHECK(symbol_index_ == symbols_.size()) << "Parsed up to " << symbol_index_
                                            << " / " << symbols_.size();
    CHECK(to_return);

    return to_return;
  }

 private:
  void NextSymbol() {
    ++symbol_index_;

    if (symbols_[symbol_index_] == SET_ID) {
      ++terminal_index_;
      CHECK(terminals_.size() > terminal_index_);
    }
  }

  bool Accept(Symbol s) {
    if (symbol_index_ == symbols_.size()) {
      return false;
    }

    if (symbols_[symbol_index_] == s) {
      NextSymbol();
      return true;
    }

    return false;
  }

  void Expect(Symbol s) {
    if (Accept(s)) {
      return;
    }

    LOG(FATAL) << "Unexpected symbol";
  }

  std::unique_ptr<TreeNode> P() {
    if (Accept(SET_ID)) {
      return std::move(terminals_[terminal_index_]);
    }

    if (Accept(LPAREN)) {
      auto then_return = Fallback();
      Expect(RPAREN);
      return then_return;
    }

    LOG(FATAL) << "Bad terminal";
    return {};
  }

  std::unique_ptr<TreeNode> Then() {
    auto lhs = P();
    CHECK(lhs != nullptr);

    auto rhs = ThenPrime();
    if (!rhs) {
      return lhs;
    }

    return nc::make_unique<ExpressionNode>(THEN, std::move(lhs),
                                           std::move(rhs));
  }

  std::unique_ptr<TreeNode> ThenPrime() {
    if (Accept(THEN)) {
      return Then();
    }

    return {};
  }

  std::unique_ptr<TreeNode> Or() {
    auto lhs = Then();
    CHECK(lhs != nullptr);

    auto rhs = OrPrime();
    if (!rhs) {
      return lhs;
    }

    return nc::make_unique<ExpressionNode>(OR, std::move(lhs), std::move(rhs));
  }

  std::unique_ptr<TreeNode> OrPrime() {
    if (Accept(OR)) {
      return Or();
    }

    return {};
  }

  std::unique_ptr<TreeNode> And() {
    auto lhs = Or();
    CHECK(lhs != nullptr);

    auto rhs = AndPrime();
    if (!rhs) {
      return lhs;
    }

    return nc::make_unique<ExpressionNode>(AND, std::move(lhs), std::move(rhs));
  }

  std::unique_ptr<TreeNode> AndPrime() {
    if (Accept(AND)) {
      return And();
    }

    return {};
  }

  std::unique_ptr<TreeNode> Fallback() {
    auto lhs = And();
    CHECK(lhs != nullptr);

    auto rhs = FallbackPrime();
    if (!rhs) {
      return lhs;
    }

    return nc::make_unique<ExpressionNode>(FALLBACK, std::move(lhs),
                                           std::move(rhs));
  }

  std::unique_ptr<TreeNode> FallbackPrime() {
    if (Accept(FALLBACK)) {
      return Fallback();
    }

    return {};
  }

  size_t symbol_index_;
  size_t terminal_index_;
  std::vector<Symbol> symbols_;
  std::vector<std::unique_ptr<TerminalNode>> terminals_;
};

int main(void) {
  std::vector<Symbol> symbols;
  std::vector<std::unique_ptr<TerminalNode>> ids;
  CHECK(
      Tokenize("visit B and visit E or avoid C then visit D", &symbols, &ids));

  LOG(ERROR) << "s " << nc::Join(symbols, ",");

  Parser p(symbols, std::move(ids));
  auto out = p.Parse();
  LOG(ERROR) << out->ToString();
}
